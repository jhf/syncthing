// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
)

// slowWatchFS blocks Watch until release is closed, like notify's
// synchronous per-directory inotify setup on a large tree.
type slowWatchFS struct {
	fs.Filesystem
	release chan struct{}
}

func (s *slowWatchFS) Watch(name string, ignore fs.Matcher, ctx context.Context, ignorePerms bool) (<-chan fs.Event, <-chan error, error) {
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	return make(chan fs.Event), make(chan error), nil
}

// The post-replay re-arm must not spend its retry budget while the watch
// backend is still being set up: it waits for the watch session's ready
// signal, and only then do attempts count. Observed on yarlung 2026-11-25:
// inotify setup for 476k directories outlasted 20 tries x (30s + 15s flush
// timeout) between 02:18 and 02:32, the re-arm gave up, and the next
// restart lost its baseline.
//
// Teeth: with the timer-only re-arm the Serve loop selects on the retry
// timer from the start, so btrfsRearmChans returns a timer channel and no
// ready channel before the backend setup finished, and the first
// assertion fails.
func TestBtrfsRearmWaitsForWatchReady(t *testing.T) {
	old := watchFlushTimeout
	t.Cleanup(func() { watchFlushTimeout = old })

	_, f, _ := barrierTestFolder(t)
	// Bounded: a pairing attempt that got past the (absent) helper would
	// wait on its barrier; the test must fail, not hang.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	// Never reach a real helper: on a btrfs host (yarlung) the pairing
	// attempt below would snapshot production (2026-11-25, two stray
	// post-replay-default-* snapshots from this test).
	t.Setenv("SYNCTHING_BTRFS_SOCK", t.TempDir()+"/no-helper.sock")
	slow := &slowWatchFS{Filesystem: f.mtimefs, release: make(chan struct{})}
	f.mtimefs = slow
	f.FSWatcherEnabled = true

	f.startWatch(ctx)
	t.Cleanup(f.stopWatch)
	// What tryArmBtrfsScanReplay leaves behind after a successful diff.
	f.btrfsRearmPending = true
	f.btrfsRearmAwaitWatch = true
	f.btrfsRearmTries = 0

	readyC, timerC := f.btrfsRearmChans()
	if timerC != nil || readyC == nil {
		t.Fatalf("before the watch is ready Serve waits on timer=%v ready=%v, want only the ready signal", timerC != nil, readyC != nil)
	}
	select {
	case <-readyC:
		t.Fatal("watch reported ready while the backend setup is still blocked")
	case <-time.After(100 * time.Millisecond):
	}

	close(slow.release)
	select {
	case <-readyC:
	case <-time.After(10 * time.Second):
		t.Fatal("watch never reported ready after the backend setup finished")
	}

	// The aggregator now accepts flushes, so the first attempt gets past
	// "watch flush unavailable" (it fails later, at the snapshot: no
	// helper here).
	watchFlushTimeout = 5 * time.Second
	ack, ok := f.requestWatchFlush(ctx)
	if !ok {
		t.Fatal("flush unavailable although the watch reported ready")
	}
	<-ack

	f.btrfsRearmWatchReady(ctx)
	if f.btrfsRearmTries != 1 {
		t.Fatalf("tries = %d after the ready-triggered attempt, want 1", f.btrfsRearmTries)
	}
	if !f.btrfsRearmPending {
		t.Fatal("re-arm gave up after one failed attempt")
	}
	if readyC, timerC := f.btrfsRearmChans(); readyC != nil || timerC == nil {
		t.Fatal("after the first failed attempt the re-arm does not retry on the timer")
	}
}

// A restarted watch session gets a fresh, open ready channel: the re-arm
// must not treat a previous session's readiness as the current one's.
func TestWatchReadyIsPerSession(t *testing.T) {
	_, f, _ := barrierTestFolder(t)
	ctx := t.Context()
	f.FSWatcherEnabled = true
	f.startWatch(ctx)
	first := f.watchReadyChan()
	select {
	case <-first:
	case <-time.After(10 * time.Second):
		t.Fatal("first watch session never ready")
	}
	f.stopWatch()

	slow := &slowWatchFS{Filesystem: f.mtimefs, release: make(chan struct{})}
	f.mtimefs = slow
	f.startWatch(ctx)
	t.Cleanup(f.stopWatch)
	second := f.watchReadyChan()
	if second == first {
		t.Fatal("restarted watch reused the previous session's ready channel")
	}
	select {
	case <-second:
		t.Fatal("restarted watch ready before its backend setup finished")
	case <-time.After(100 * time.Millisecond):
	}
	close(slow.release)
}
