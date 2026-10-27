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
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
)

func retryPeerFixture(t *testing.T) (*testModel, *sendReceiveFolder, *fakeConnection, *time.Time) {
	t.Helper()
	m, f := setupSendReceiveFolder(t)
	now := time.Now()
	f.retry = newPullRetryTracker(f.pullBasePause, f.churn)
	f.retry.budget = new(pullRetryBudget)
	f.retry.now = func() time.Time { return now }
	t.Cleanup(f.retry.clearAll)
	conn := newFakeConnection(device1, m)
	conn.folder = f.folderID
	id := conn.ConnectionID()
	m.mut.Lock()
	m.connections[id] = conn
	m.deviceConnIDs[device1] = []string{id}
	m.remoteFolderStates[device1] = map[string]remoteFolderState{f.folderID: remoteFolderValid}
	m.deviceDownloads[device1] = newDeviceDownloadState()
	m.mut.Unlock()
	return m, f, conn, &now
}

type retryFailUpdateDB struct {
	db.DB
	fail     error
	countErr error
}

func (d *retryFailUpdateDB) CountNeed(folder string, device protocol.DeviceID) (db.Counts, error) {
	if d.countErr != nil {
		return db.Counts{}, d.countErr
	}
	return d.DB.CountNeed(folder, device)
}

func (d *retryFailUpdateDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	if d.fail != nil {
		return d.fail
	}
	return d.DB.Update(folder, device, files, opts...)
}

func TestPullRetryFailedCommitRetainsStateAndDrains(t *testing.T) {
	_, f, _, _ := retryPeerFixture(t)
	f.retry.beginWalk()
	f.retry.failure("held", "remote_no_such_file", "block_request", "real failure")
	before, err := f.GetLastFile()
	must(t, err)
	sentinel := errors.New("injected database failure")
	d := &retryFailUpdateDB{DB: f.db, fail: sentinel}
	f.db = d
	jobs := make(chan dbUpdateJob)
	done := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(jobs); done <- err }()
	for i := range dbUpdateBatchSizeFiles + 10 {
		name := "held"
		if i > 0 {
			name = "other"
		}
		select {
		case jobs <- dbUpdateJob{protocol.FileInfo{Name: name, Version: protocol.Vector{}.Update(1)}, dbUpdateHandleFile}:
		case <-t.Context().Done():
			t.Fatal("failed updater stopped draining")
		}
	}
	close(jobs)
	if err := <-done; !errors.Is(err, sentinel) {
		t.Fatalf("lost DB error: %v", err)
	}
	if len(f.retry.entries) != 1 {
		t.Fatal("failed batch cleared unknown per-file outcome")
	}
	after, err := f.GetLastFile()
	must(t, err)
	if after != before {
		t.Fatalf("failed commit emitted ReceivedFile: %+v", after)
	}
	// A later invocation can commit unrelated work, without assuming the
	// failed batch committed any particular file.
	d.fail = nil
	jobs = make(chan dbUpdateJob, 1)
	jobs <- dbUpdateJob{protocol.FileInfo{Name: "stable", Version: protocol.Vector{}.Update(1)}, dbUpdateHandleFile}
	close(jobs)
	_, err = f.dbUpdaterRoutine(jobs)
	must(t, err)
	last, err := f.GetLastFile()
	must(t, err)
	if last.Filename != "stable" || len(f.retry.entries) != 1 {
		t.Fatalf("later commit did not progress independently: %+v", last)
	}
	_, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, "stable")
	must(t, err)
	if !ok {
		t.Fatal("stable file not committed")
	}
	jobs = make(chan dbUpdateJob, 1)
	jobs <- dbUpdateJob{protocol.FileInfo{Name: "held", Version: protocol.Vector{}.Update(1)}, dbUpdateHandleFile}
	close(jobs)
	_, err = f.dbUpdaterRoutine(jobs)
	must(t, err)
	if len(f.retry.entries) != 0 {
		t.Fatal("confirmed successful commit did not clear cooldown")
	}
}
func retryPeerIndex(t *testing.T, m *testModel, f *sendReceiveFolder, conn *fakeConnection) {
	t.Helper()
	files := make([]protocol.FileInfo, len(conn.files))
	for i, fi := range conn.files {
		files[i] = prepareFileInfoForIndex(fi)
	}
	must(t, m.sdb.Update(f.folderID, device1, files))
}
func TestPullRetryPeerStableSiblingAndOwnDeadline(t *testing.T) {
	m, f, conn, now := retryPeerFixture(t)
	conn.addFile("hot", 0o644, protocol.FileInfoTypeFile, []byte("churn"))
	conn.addFile("stable", 0o644, protocol.FileInfoTypeFile, []byte("stable sibling"))
	retryPeerIndex(t, m, f, conn)
	var hot, stable atomic.Int64
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		if req.Name == "hot" {
			hot.Add(1)
			return nil, protocol.ErrNoSuchFile
		}
		stable.Add(1)
		return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
	})
	success, err := f.pull(t.Context())
	must(t, err)
	if success || hot.Load() != 1 || stable.Load() != 1 {
		t.Fatalf("success=%v hot=%d stable=%d", success, hot.Load(), stable.Load())
	}
	data, err := f.mtimefs.Open("stable")
	must(t, err)
	data.Close()
	if s := f.RetryStatus(); s.deferredNeeded != 1 || s.attempted != 2 || !s.completeNeedWalk {
		t.Fatalf("status %+v", s)
	}
	for range 5 {
		success, err = f.pull(t.Context())
		must(t, err)
		if success || hot.Load() != 1 || !f.RetryStatus().pureDeferral() {
			t.Fatal("announcement bypass or false success")
		}
	}
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		hot.Add(1)
		return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
	})
	*now = now.Add(f.pullBasePause())
	success, err = f.pull(t.Context())
	must(t, err)
	if !success || hot.Load() != 2 || len(f.retry.entries) != 0 {
		t.Fatalf("deadline repair success=%v hot=%d entries=%d", success, hot.Load(), len(f.retry.entries))
	}
}
func TestPullRetryMetadataNeedWalkAndPopGates(t *testing.T) {
	for _, shortcut := range []bool{false, true} {
		t.Run(map[bool]string{false: "transfer", true: "shortcut"}[shortcut], func(t *testing.T) {
			m, f, conn, _ := retryPeerFixture(t)
			logger := new(churnEventLogger)
			f.evLogger = logger
			conn.addFile("held", 0o644, protocol.FileInfoTypeFile, []byte("payload"))
			retryPeerIndex(t, m, f, conn)
			if shortcut {
				local := conn.files[0]
				local.Version = protocol.Vector{}
				must(t, m.sdb.Update(f.folderID, protocol.LocalDeviceID, []protocol.FileInfo{local}))
			}
			f.retry.begin()
			f.retry.beginWalk()
			f.retry.failure("held", "local_modified", "shortcut_check", "previous real error")
			f.retry.beginWalk()
			_, _, _, _, err := f.processNeeded(t.Context(), make(chan dbUpdateJob, 10), make(chan copyBlocksState, 10), make(chan string, 10))
			must(t, err)
			_, pending, _ := f.queue.Jobs(1, 10)
			if len(pending) != 1 || pending[0] != "held" || len(f.tempPullErrors) != 0 || f.churn.total.values[churnIndex("attempts")] != 0 {
				t.Fatal("need-walk gate attempted held work")
			}
			f.queue.Reset()
			f.queue.Push("held", 7, time.Time{})
			must(t, f.processQueue(t.Context(), nil, nil, make(chan copyBlocksState, 10), make(chan dbUpdateJob, 10), make(chan string, 10)))
			if len(f.queue.progress) != 0 || len(f.queue.held) != 1 {
				t.Fatal("pop gate lost held membership")
			}
			if len(logger.finished) != 0 {
				t.Fatal("held file emitted synthetic ItemFinished")
			}
		})
	}
}
func TestPullRetryDeleteCreateEndings(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[present], func(t *testing.T) {
			m, f, conn, now := retryPeerFixture(t)
			conn.addFile("file", 0o644, protocol.FileInfoTypeFile, []byte("first"))
			retryPeerIndex(t, m, f, conn)
			conn.RequestReturns(nil, protocol.ErrNoSuchFile)
			success, err := f.pull(t.Context())
			must(t, err)
			if success {
				t.Fatal("first failure succeeded")
			}
			deadline := f.retry.entries["file"].nextEligible
			conn.deleteFile("file")
			retryPeerIndex(t, m, f, conn)
			if present {
				conn.updateFile("file", 0o644, protocol.FileInfoTypeFile, []byte("final content"))
				retryPeerIndex(t, m, f, conn)
				success, err = f.pull(t.Context())
				must(t, err)
				if success || !f.retry.entries["file"].nextEligible.Equal(deadline) {
					t.Fatal("new version bypassed deadline")
				}
				conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
					return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
				})
				*now = deadline
				success, err = f.pull(t.Context())
				must(t, err)
				if !success {
					t.Fatal("final present did not converge")
				}
				fd, err := f.mtimefs.Open("file")
				must(t, err)
				var got bytes.Buffer
				_, err = got.ReadFrom(fd)
				must(t, err)
				fd.Close()
				if got.String() != "final content" {
					t.Fatalf("wrong final content %q", got.String())
				}
			} else {
				success, err = f.pull(t.Context())
				must(t, err)
				if !success || len(f.retry.entries) != 0 {
					t.Fatal("tombstone gated or state resurrected")
				}
			}
		})
	}
}
func TestPullRetryConfigurationScope(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	for _, kind := range []config.FolderType{config.FolderTypeSendReceive, config.FolderTypeReceiveOnly, config.FolderTypeReceiveEncrypted} {
		cfg := f.FolderConfiguration
		cfg.Type = kind
		runner := newSendReceiveFolder(m.model, f.ignores, cfg, nil, f.evLogger, f.ioLimiter).(*sendReceiveFolder)
		if (runner.retry != nil) != (kind != config.FolderTypeReceiveEncrypted) {
			t.Fatalf("wrong scope %v", kind)
		}
		runner.ClearRetryState()
	}
	t.Setenv("STPULLRETRYCOOLDOWN", "0")
	runner := newSendReceiveFolder(m.model, f.ignores, f.FolderConfiguration, nil, f.evLogger, f.ioLimiter).(*sendReceiveFolder)
	if runner.retry != nil {
		t.Fatal("kill switch ignored")
	}
}

func TestPullRetryUpdateDuringTransfer(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "superseded", true: "deleted"}[deleted], func(t *testing.T) {
			m, f, conn, now := retryPeerFixture(t)
			conn.addFile("file", 0o644, protocol.FileInfoTypeFile, []byte("old content"))
			retryPeerIndex(t, m, f, conn)
			entered, release := make(chan struct{}), make(chan struct{})
			conn.RequestCalls(func(ctx context.Context, _ *protocol.Request) ([]byte, error) {
				close(entered)
				select {
				case <-release:
					return nil, protocol.ErrNoSuchFile
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			done := make(chan error, 1)
			go func() { _, err := f.pull(t.Context()); done <- err }()
			<-entered
			if deleted {
				conn.deleteFile("file")
			} else {
				conn.updateFile("file", 0o644, protocol.FileInfoTypeFile, []byte("new content"))
			}
			retryPeerIndex(t, m, f, conn)
			close(release)
			must(t, <-done)
			outcome := "superseded"
			if deleted {
				outcome = "tombstoned"
			}
			if f.churn.total.values[churnIndex("need.failure_sample."+outcome)] != 1 {
				t.Fatalf("missing raced current-state observation %s", outcome)
			}
			conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
				return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
			})
			if !deleted {
				*now = now.Add(f.pullBasePause())
			}
			success, err := f.pull(t.Context())
			must(t, err)
			if !success || len(f.retry.entries) != 0 {
				t.Fatal("current state failed to converge")
			}
			if deleted {
				if _, err := f.mtimefs.Lstat("file"); err == nil {
					t.Fatal("deleted file resurrected")
				}
			}
		})
	}
}

func TestPullRetryRetainsReusableTemporaryBlocks(t *testing.T) {
	m, f, conn, now := retryPeerFixture(t)
	data := append(bytes.Repeat([]byte{41}, protocol.MinBlockSize), bytes.Repeat([]byte{83}, protocol.MinBlockSize)...)
	conn.addFile("file", 0o644, protocol.FileInfoTypeFile, data)
	retryPeerIndex(t, m, f, conn)
	var first, second atomic.Int64
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		if req.Offset == 0 {
			first.Add(1)
			return data[:req.Size], nil
		}
		second.Add(1)
		return nil, protocol.ErrNoSuchFile
	})
	success, err := f.pull(t.Context())
	must(t, err)
	if success || first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("first transfer counts %d %d", first.Load(), second.Load())
	}
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		if req.Offset == 0 {
			first.Add(1)
		} else {
			second.Add(1)
		}
		return data[req.Offset : req.Offset+int64(req.Size)], nil
	})
	*now = now.Add(f.pullBasePause())
	success, err = f.pull(t.Context())
	must(t, err)
	if !success || first.Load() != 1 || second.Load() != 2 {
		t.Fatalf("retry discarded good temporary blocks: success=%v first=%d second=%d", success, first.Load(), second.Load())
	}
}

type retryStubPuller struct {
	status pullRetryStatus
	err    error
	calls  int
}

func (p *retryStubPuller) pull(context.Context) (bool, error) { p.calls++; return false, p.err }
func (p *retryStubPuller) RetryStatus() pullRetryStatus       { return p.status }
func (p *retryStubPuller) ClearRetryState()                   { p.status = pullRetryStatus{} }

func TestPullRetryTimerAndScanBoundary(t *testing.T) {
	m, f, conn, _ := retryPeerFixture(t)
	conn.addFile("file", 0o644, protocol.FileInfoTypeFile, []byte("needed"))
	retryPeerIndex(t, m, f, conn)
	stub := &retryStubPuller{status: pullRetryStatus{completeNeedWalk: true, deferredNeeded: 1, nextEligible: time.Now().Add(time.Hour)}}
	f.puller = stub
	pause := f.pullBasePause() * 2
	f.pullPause = pause
	for {
		select {
		case <-f.pullScheduled:
			continue
		default:
			goto drained
		}
	}
drained:
	f.pullFailTimer.Reset(0)
	ran, err := f.serviceQueuedPull(t.Context())
	must(t, err)
	if !ran || stub.calls != 1 || f.pullPause != pause {
		t.Fatal("chunk-boundary retry missing or pure deferral doubled pause")
	}
	stub.err = errors.New("unrelated failure")
	f.pullFailTimer.Reset(0)
	ran, err = f.serviceQueuedPull(t.Context())
	if !ran || !errors.Is(err, stub.err) || f.pullPause != 2*pause {
		t.Fatal("ordinary error lost retry backoff")
	}
	// A stale completed status must not override an early health/DB error.
	stub.err = nil
	sentinel := errors.New("count failed")
	f.db = &retryFailUpdateDB{DB: f.db, countErr: sentinel}
	_, err = f.folder.pull(t.Context())
	if !errors.Is(err, sentinel) || stub.calls != 2 {
		t.Fatal("early DB error dispatched stale retry status")
	}
	if f.pullFailTimer.Stop() {
		t.Fatal("early DB error armed old retry deadline")
	}
}

func TestPullRetryAllHeldWakesWithoutAnnouncements(t *testing.T) {
	m, f, conn, _ := retryPeerFixture(t)
	f.retry.now = time.Now
	f.retry.pause = func() time.Duration { return 150 * time.Millisecond }
	conn.addFile("file", 0o644, protocol.FileInfoTypeFile, []byte("eventual content"))
	retryPeerIndex(t, m, f, conn)
	conn.RequestReturns(nil, protocol.ErrNoSuchFile)
	success, err := f.folder.pull(t.Context())
	must(t, err)
	if success || f.RetryStatus().deferredNeeded != 1 {
		t.Fatal("all-held folder falsely synced")
	}
	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
	})
	select {
	case <-f.pullFailTimer.C:
	case <-time.After(2 * time.Second):
		t.Fatal("own retry timer never woke")
	}
	must(t, f.serviceRetryPull(t.Context()))
	count, err := f.db.CountNeed(f.folderID, protocol.LocalDeviceID)
	must(t, err)
	if count.TotalItems() != 0 || len(f.retry.entries) != 0 {
		t.Fatal("timer did not admit and settle without another index")
	}
	f.pullFailTimer.Stop()
}

func TestPullRetryAdmissionControlMeasurement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
			m, f, conn, _ := retryPeerFixture(t)
			if !enabled {
				f.retry = nil
			}
			conn.addFile("hot", 0o644, protocol.FileInfoTypeFile, []byte("churn"))
			conn.addFile("stable", 0o644, protocol.FileInfoTypeFile, []byte("stable"))
			retryPeerIndex(t, m, f, conn)
			var requests, networkBytes atomic.Int64
			conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
				requests.Add(1)
				if req.Name == "hot" {
					return nil, protocol.ErrNoSuchFile
				}
				networkBytes.Add(int64(req.Size))
				return conn.fileData[req.Name][req.Offset : req.Offset+int64(req.Size)], nil
			})
			for range 6 {
				success, err := f.pull(t.Context())
				must(t, err)
				if success {
					t.Fatal("churn falsely synced")
				}
			}
			want := uint64(8) // First invocation retries hot after stable DB progress.
			if enabled {
				want = 2
			}
			attempts := f.churn.total.values[churnIndex("attempts")]
			if attempts != want || uint64(requests.Load()) != want {
				t.Fatalf("attempts=%d requests=%d want=%d", attempts, requests.Load(), want)
			}
			t.Logf("enabled=%v attempts=%d block_requests=%d bytes=%d failures=%d warn_suppressed=%d pending=%d", enabled, attempts, requests.Load(), networkBytes.Load(), f.churn.total.values[churnIndex("failures")], f.churn.total.values[churnIndex("warn_suppressed")], f.RetryStatus().deferredNeeded)
		})
	}
}

func TestPullRetryAlternatePeerSuccessDoesNotGate(t *testing.T) {
	m, f, conn, _ := retryPeerFixture(t)
	payload := []byte("alternate source content")
	conn.addFile("file", 0o644, protocol.FileInfoTypeFile, payload)
	retryPeerIndex(t, m, f, conn)
	second := newFakeConnection(device2, m)
	id := second.ConnectionID()
	m.mut.Lock()
	m.connections[id] = second
	m.deviceConnIDs[device2] = []string{id}
	m.remoteFolderStates[device2] = map[string]remoteFolderState{f.folderID: remoteFolderValid}
	m.deviceDownloads[device2] = newDeviceDownloadState()
	m.mut.Unlock()
	files := []protocol.FileInfo{prepareFileInfoForIndex(conn.files[0])}
	must(t, m.sdb.Update(f.folderID, device2, files))
	var requests atomic.Int64
	request := func(_ context.Context, req *protocol.Request) ([]byte, error) {
		if requests.Add(1) == 1 {
			return nil, protocol.ErrNoSuchFile
		}
		return payload[req.Offset : req.Offset+int64(req.Size)], nil
	}
	conn.RequestCalls(request)
	second.RequestCalls(request)
	success, err := f.pull(t.Context())
	must(t, err)
	if !success || requests.Load() != 2 || len(f.retry.entries) != 0 || f.churn.total.values[churnIndex("failures")] != 0 || f.churn.total.values[churnIndex("block_failures")] != 1 {
		t.Fatal("candidate rejection became terminal cooldown")
	}
}

func TestPullRetryPopObsoleteClears(t *testing.T) {
	for _, kind := range []string{"missing", "deleted", "directory", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			m, f, _, _ := retryPeerFixture(t)
			f.retry.failure("file", "remote_no_such_file", "block_request", "real failure")
			fi := protocol.FileInfo{Name: "file", Version: protocol.Vector{}.Update(1)}
			switch kind {
			case "deleted":
				fi.Deleted = true
			case "directory":
				fi.Type = protocol.FileInfoTypeDirectory
			case "invalid":
				fi.SetIgnored()
			}
			if kind != "missing" {
				must(t, m.sdb.Update(f.folderID, device1, []protocol.FileInfo{fi}))
			}
			f.queue.Push("file", 1, time.Time{})
			must(t, f.processQueue(t.Context(), nil, nil, make(chan copyBlocksState, 1), make(chan dbUpdateJob, 1), make(chan string, 1)))
			if len(f.retry.entries) != 0 || len(f.queue.held) != 0 || len(f.queue.progress) != 0 {
				t.Fatal("obsolete refreshed item retained retry state")
			}
			if f.churn.total.values[churnIndex("queue.clear_obsolete")] != 1 || f.churn.total.values[churnIndex("queue.retry_clear_obsolete")] != 1 {
				t.Fatal("queue and tracker clears did not reconcile independently")
			}
		})
	}
}

func TestPullRetryReceiveOnlyLocalGuard(t *testing.T) {
	m, base, conn, _ := retryPeerFixture(t)
	cfg := base.FolderConfiguration
	cfg.Type = config.FolderTypeReceiveOnly
	ro := newReceiveOnlyFolder(m.model, base.ignores, cfg, nil, base.evLogger, base.ioLimiter).(*receiveOnlyFolder)
	f := ro.sendReceiveFolder
	close(f.initialScanFinished)
	close(f.done)
	t.Cleanup(f.ClearRetryState)
	conn.addFile("file", 0o644, protocol.FileInfoTypeFile, []byte("indexed payload"))
	retryPeerIndex(t, m, f, conn)
	local := conn.files[0]
	local.Version = protocol.Vector{}
	must(t, m.sdb.Update(f.folderID, protocol.LocalDeviceID, []protocol.FileInfo{local}))
	// Disk is missing despite the indexed equal blocks. Receive-only's
	// existing local-change guard must still fail, then A holds the shortcut.
	success, err := f.pull(t.Context())
	must(t, err)
	if success || len(f.retry.entries) != 1 || f.localFlags != protocol.FlagLocalReceiveOnly {
		t.Fatal("receive-only guard bypassed")
	}
	before := f.churn.total.values[churnIndex("attempts")]
	success, err = f.pull(t.Context())
	must(t, err)
	if success || f.churn.total.values[churnIndex("attempts")] != before {
		t.Fatal("receive-only shortcut cooldown missed")
	}
}

func TestPullRetryScanWalkAndSweepServiceDueTimer(t *testing.T) {
	old := scanChunkBudget
	defer func() { scanChunkBudget = old }()
	scanChunkBudget = 0
	for _, sweep := range []bool{false, true} {
		t.Run(map[bool]string{false: "walk", true: "sweep"}[sweep], func(t *testing.T) {
			m, f, conn, _ := retryPeerFixture(t)
			must(t, f.mtimefs.MkdirAll("tree", 0o755))
			writeFile(t, f.mtimefs, "tree/file", []byte("local"))
			must(t, f.scanSubdirs(t.Context(), []string{"tree"}))
			conn.addFile("needed", 0o644, protocol.FileInfoTypeFile, []byte("remote"))
			retryPeerIndex(t, m, f, conn)
			stub := &retryStubPuller{status: pullRetryStatus{completeNeedWalk: true, deferredNeeded: 1, nextEligible: time.Now().Add(time.Hour)}}
			f.puller = stub
			for {
				select {
				case <-f.pullScheduled:
					continue
				default:
					goto drained
				}
			}
		drained:
			f.pullFailTimer.Reset(0)
			if sweep {
				must(t, f.ioLimiter.TakeWithContext(t.Context(), 1))
				_, err := f.scanSubdirsDeletedAndIgnored(t.Context(), []string{"tree"}, f.newScanBatch())
				f.ioLimiter.Give(1)
				must(t, err)
			} else {
				must(t, f.scanSubdirs(t.Context(), []string{"tree"}))
			}
			if stub.calls == 0 {
				t.Fatal("scan chunk starved due retry timer")
			}
			if f.pullPause != f.pullBasePause() {
				t.Fatal("scan retry doubled pause on pure deferral")
			}
			f.pullFailTimer.Stop()
		})
	}
}
