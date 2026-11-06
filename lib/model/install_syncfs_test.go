// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

// countingSyncFS counts File.Sync calls on files opened through it, so a test
// can tell per-temp fsyncs apart from group-level syncfs barriers.
type countingSyncFS struct {
	fs.Filesystem
	syncs atomic.Int64
}

func (c *countingSyncFS) Open(name string) (fs.File, error) {
	fd, err := c.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	return &countingSyncFile{File: fd, n: &c.syncs}, nil
}

type countingSyncFile struct {
	fs.File
	n *atomic.Int64
}

func (c *countingSyncFile) Sync() error {
	c.n.Add(1)
	return c.File.Sync()
}

// runSyncfsInstallGroup pulls n files through the real copier, finishers and
// updater with syncfs mode forced on and installSyncfsFn replaced by fn.
func runSyncfsInstallGroup(t *testing.T, n int, fn func(string) error) (*sendReceiveFolder, *countingSyncFS, []byte) {
	t.Helper()
	return runSyncfsInstallGroupPrimed(t, n, nil, fn)
}

// runSyncfsInstallGroupPrimed is runSyncfsInstallGroup with a hook that runs
// on the folder before the pipeline starts (patch/71 primes cost estimates).
func runSyncfsInstallGroupPrimed(t *testing.T, n int, prime func(*sendReceiveFolder), fn func(string) error) (*sendReceiveFolder, *countingSyncFS, []byte) {
	t.Helper()
	f, fi, data, _ := intentFixture(t, "syncfs-fixture")
	if prime != nil {
		prime(f)
	}
	installSyncfsForTest = true
	t.Cleanup(func() { installSyncfsForTest = false })
	if !f.installSyncfs() {
		t.Fatal("install syncfs mode not active")
	}
	counter := &countingSyncFS{Filesystem: f.mtimefs}
	f.mtimefs = counter
	old := installSyncfsFn
	t.Cleanup(func() { installSyncfsFn = old })
	installSyncfsFn = fn
	f.Copiers = 8
	d := &growthInstallDB{DB: f.db}
	f.db = d
	jobs := make(chan dbUpdateJob, 8)
	finish := make(chan *sharedPullerState, 8)
	pulls := make(chan pullBlockState, n)
	copies, copyWG := startCopier(t.Context(), f, pulls, finish)
	scans := make(chan string, n)
	var finishWG sync.WaitGroup
	for range 8 {
		finishWG.Go(func() { f.finisherRoutine(t.Context(), finish, jobs, scans) })
	}
	updater := make(chan error, 1)
	go func() { _, err := f.dbUpdaterRoutine(t.Context(), jobs); updater <- err }()
	counter.syncs.Store(0)
	for i := range n {
		file := fi
		file.Name = fmt.Sprintf("syncfs-%03d", i)
		file.Blocks = append([]protocol.BlockInfo(nil), fi.Blocks...)
		writeFile(t, f.mtimefs, fs.TempName(file.Name), data)
		must(t, f.handleFile(t.Context(), file, copies))
	}
	close(copies)
	copyWG.Wait()
	close(finish)
	finishWG.Wait()
	close(jobs)
	must(t, <-updater)
	for i := range n {
		name := fmt.Sprintf("syncfs-%03d", i)
		indexed, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, name)
		must(t, err)
		if !ok || indexed.LocalFlags&protocol.FlagLocalInstallPending != 0 {
			t.Fatalf("incomplete install: %s", name)
		}
		if got := readInstallBytes(t, f, name); string(got) != string(data) {
			t.Fatalf("wrong bytes: %s", name)
		}
	}
	return f, counter, data
}

// In syncfs mode a large group pays two filesystem barriers (before the
// durable commit and after the renames), never one fsync per temp file.
// Groups below installSyncfsMinGroup fsync their temps individually.
func TestInstallAfterCommitSyncfsGroupBarriers(t *testing.T) {
	const n = 200
	var calls atomic.Int64
	_, counter, _ := runSyncfsInstallGroup(t, n, func(string) error { calls.Add(1); return nil })
	t.Logf("files=%d syncfs=%d perFileSyncs=%d", n, calls.Load(), counter.syncs.Load())
	if got := counter.syncs.Load(); got > 2*installSyncfsMinGroup {
		t.Fatalf("syncfs mode performed %d per-file fsyncs for %d files, want only small-group ones (<= %d)", got, n, 2*installSyncfsMinGroup)
	}
	if c := calls.Load(); c == 0 || c > int64(n)/4 {
		t.Fatalf("syncfs barriers = %d for %d files, want grouped (1..%d)", c, n, n/4)
	}
}

// A failing syncfs must never let the commit proceed over unsynced temps:
// every temp is fsynced individually instead.
func TestInstallAfterCommitSyncfsFailureFallsBack(t *testing.T) {
	const n = 20
	_, counter, _ := runSyncfsInstallGroup(t, n, func(string) error { return errors.New("syncfs unavailable") })
	if got := counter.syncs.Load(); got < n {
		t.Fatalf("syncfs failure fell back to %d fsyncs, want at least one per temp (%d)", got, n)
	}
}
