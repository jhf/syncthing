// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

//go:build darwin

package model

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/protocol"
)

// macOS group barrier (patch/74): a group of temps and directories gets a
// plain fsync each and exactly ONE F_FULLFSYNC, instead of one F_FULLFSYNC
// per item. Real files on the real filesystem; only the flush is counted.
func TestDarwinGroupBarrierOneFullFsync(t *testing.T) {
	root := t.TempDir()
	var paths []string
	for i, name := range []string{"a", "b", "c", "d"} {
		p := filepath.Join("sub", name)
		if i == 0 {
			if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(root, p), []byte("content "+name), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	paths = append(paths, "sub")

	var flushes atomic.Int64
	old := fullFsyncFn
	t.Cleanup(func() { fullFsyncFn = old })
	fullFsyncFn = func(fd *os.File) error { flushes.Add(1); return old(fd) }

	if err := installBarrierFn(root, paths); err != nil {
		t.Fatal(err)
	}
	if got := flushes.Load(); got != 1 {
		t.Fatalf("group of %d items made %d F_FULLFSYNC calls, want exactly 1", len(paths), got)
	}
	// An empty group still flushes once (the caller only asks when needed).
	if err := installBarrierFn(root, nil); err != nil {
		t.Fatal(err)
	}
	if got := flushes.Load(); got != 2 {
		t.Fatalf("empty group: %d flushes total, want 2", got)
	}
	// A missing item is an error, never a silent partial barrier.
	if err := installBarrierFn(root, []string{"sub/missing"}); err == nil {
		t.Fatal("missing item must fail the barrier so the fence falls back")
	}
}

// With the costs measured on this Mac (per-item F_FULLFSYNC 6.1 ms, group
// barrier 5.6 ms), the cost model groups from n=2 once warmed up.
func TestDarwinCostModelGroupsFromTwo(t *testing.T) {
	var c installSyncCost
	for range installCostWarmup {
		c.observeBarrier(5600 * time.Microsecond)
		c.observeItems(6100*time.Microsecond, 1)
	}
	for _, n := range []int{2, 4, 32} {
		c.fences = 0
		if got := c.choose(n); got != fenceBarrier {
			t.Errorf("n=%d: got %v, want the group barrier", n, got)
		}
	}
}

// The install path really is in barrier mode on macOS with a basic
// filesystem (before patch/74 only Linux had a barrier).
func TestDarwinInstallBarrierEnabled(t *testing.T) {
	f, _, _, _ := physicalInstallFixture(t, "barrier-mode")
	t.Setenv("STSYNCMODE", "")
	if !f.installBarrier() {
		t.Fatal("macOS with a basic filesystem must use the install group barrier")
	}
	t.Setenv("STSYNCMODE", "fsync")
	if f.installBarrier() {
		t.Fatal("STSYNCMODE=fsync must turn the barrier off")
	}
}

// Crash after a real FULL commit whose temp was made durable by the macOS
// group barrier, not by a per-temp F_FULLFSYNC: recovery after reopening
// must publish the exact sender content with the sender's version. The
// physical fixture is a basic filesystem, so barrier mode is on.
func TestDarwinBarrierCrashRecovery(t *testing.T) {
	var flushes atomic.Int64
	oldFull := fullFsyncFn
	t.Cleanup(func() { fullFsyncFn = oldFull })
	fullFsyncFn = func(fd *os.File) error { flushes.Add(1); return oldFull(fd) }

	f, fi, data, temp := physicalInstallFixture(t, "macpending")
	if !f.installBarrier() {
		t.Fatal("precondition: barrier mode on a basic filesystem")
	}
	// Force the barrier for this group: costs as measured on this Mac.
	for range installCostWarmup {
		f.installCost.pre.observeBarrier(5600 * time.Microsecond)
		f.installCost.pre.observeItems(6100*time.Microsecond, 1)
	}
	dir := t.TempDir()
	original, err := sqlite.Open(dir)
	must(t, err)
	f.db = original
	jobs := make(chan dbUpdateJob, 1)
	must(t, f.performFinish(fi, protocol.FileInfo{}, false, temp, jobs, make(chan string, 8)))
	close(jobs)
	crash := errors.New("simulated death after real FULL commit, before rename")
	f.db = &installBarrierDB{DB: original, after: func() error { return crash }}
	_, err = f.dbUpdaterRoutine(t.Context(), jobs)
	if !errors.Is(err, crash) {
		t.Fatal(err)
	}
	if flushes.Load() != 1 {
		t.Fatalf("precommit fence: %d F_FULLFSYNC, want exactly 1 (group barrier)", flushes.Load())
	}
	must(t, original.Close())

	reopened, err := sqlite.Open(dir)
	must(t, err)
	defer reopened.Close()
	restarted := newSendReceiveFolder(f.model, f.ignores, f.FolderConfiguration, nil, f.evLogger, f.ioLimiter).(*sendReceiveFolder)
	restarted.db = reopened
	restarted.mtimefs = f.mtimefs
	must(t, restarted.RecoverPendingInstalls(t.Context()))
	got, ok, err := reopened.GetDeviceFile(f.ID, protocol.LocalDeviceID, fi.Name)
	must(t, err)
	if !ok || !got.Version.Equal(fi.Version) || got.LocalFlags&protocol.FlagLocalInstallPending != 0 || !bytes.Equal(readInstallBytes(t, restarted, fi.Name), data) {
		t.Fatalf("recovery after barrier-durable temp failed: ok=%v flags=%v", ok, got.LocalFlags)
	}
}
