// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !(solaris && !cgo) && !(darwin && !cgo) && !(darwin && kqueue)
// +build !solaris cgo
// +build !darwin cgo
// +build !darwin !kqueue

package fs

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/syncthing/notify"
)

// politeBackend delivers names into backendChan the way a backend slower
// than the pump would: it never fills the channel (so nothing is lost and
// no overflow is signalled), waiting for room instead. It reports whether
// every event was delivered before the deadline.
func politeBackend(backendChan chan notify.EventInfo, names []string, deadline time.Duration) bool {
	stop := time.Now().Add(deadline)
	for _, n := range names {
		for len(backendChan) >= cap(backendChan)-1 {
			if time.Now().After(stop) {
				return false
			}
			runtime.Gosched()
		}
		backendChan <- fakeEventInfo(filepath.Join(testDirAbs, n))
	}
	return true
}

func setWatchBacklogLimits(t *testing.T, boot, steady, coarse int) func() []string {
	t.Helper()
	ob, os, oc, ot, ol := watchBacklogBootLimit, watchBacklogSteadyLimit, watchBacklogCoarseLimit, watchBacklogLogThreshold, watchBacklogLog
	var mut sync.Mutex
	var lines []string
	watchBacklogBootLimit, watchBacklogSteadyLimit, watchBacklogCoarseLimit, watchBacklogLogThreshold = boot, steady, coarse, 1
	watchBacklogLog = func(msg string, args ...any) {
		mut.Lock()
		lines = append(lines, msg+" "+fmt.Sprint(args...))
		mut.Unlock()
	}
	t.Cleanup(func() {
		watchBacklogBootLimit, watchBacklogSteadyLimit, watchBacklogCoarseLimit, watchBacklogLogThreshold, watchBacklogLog = ob, os, oc, ot, ol
	})
	return func() []string {
		mut.Lock()
		defer mut.Unlock()
		return append([]string(nil), lines...)
	}
}

func runWatchLoop(t *testing.T, fs *BasicFilesystem, backendChan chan notify.EventInfo) chan Event {
	t.Helper()
	outChan := make(chan Event)
	errChan := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		fs.watchLoop(ctx, ".", []string{testDirAbs}, backendChan, outChan, errChan, fakeMatcher{})
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return outChan
}

func receiveN(t *testing.T, outChan chan Event, n int) []string {
	t.Helper()
	var got []string
	for len(got) < n {
		select {
		case ev := <-outChan:
			got = append(got, ev.Name)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out after %d of %d events: %q", len(got), n, got)
		}
	}
	return got
}

// TestWatchBacklogAbsorbsBurstWhileConsumerBlocked pins the backlog: with
// the consumer blocked (a long scan holds the aggregator), a burst of
// 10x the backend buffer must be drained from the backend channel without
// loss, then delivered in order with no full-folder escalation. Measured
// 2026-11-24: a boot journal replay pushed 69,773 events into the
// 65,536-entry channel 2 s after start and the whole folder was rescanned.
// Teeth: before the pump, nothing drained the channel while the consumer
// was blocked, so the backend could not deliver (in production: notify
// dropped events and the loop escalated to the root).
func TestWatchBacklogAbsorbsBurstWhileConsumerBlocked(t *testing.T) {
	logLines := setWatchBacklogLimits(t, 1<<20, 1<<20, 256)
	fs := newBasicFilesystem(testDirAbs)

	backendChan := make(chan notify.EventInfo, backendBuffer)
	outChan := runWatchLoop(t, fs, backendChan)

	n := 10 * backendBuffer
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("f%04d", i)
	}
	// Nobody reads outChan while the backend delivers.
	if !politeBackend(backendChan, names, 3*time.Second) {
		t.Fatal("backend could not deliver: the backend channel was not drained while the consumer was blocked")
	}

	got := receiveN(t, outChan, n)
	if !slices.Equal(got, names) {
		t.Fatalf("delivered events differ from the burst (first %q, want %q)", got[:3], names[:3])
	}
	drained := false
	for _, l := range logLines() {
		if len(l) >= len("Watch backlog drained") && l[:len("Watch backlog drained")] == "Watch backlog drained" {
			drained = true
		}
	}
	if !drained {
		t.Errorf("no 'Watch backlog drained' line, got %q", logLines())
	}
}

// TestWatchBacklogOverLimitScansSubtrees pins the refusal policy: past the
// backlog limit, events are not dropped and do not escalate to the root -
// they coarsen into the directories they touched, which are emitted as
// scan events once the queue ahead has drained. A barrier canary arriving
// during the overflow is delivered after them (FIFO for the barrier).
// Teeth: before the backlog, the same burst overflowed the channel and was
// replaced by one full-folder event.
func TestWatchBacklogOverLimitScansSubtrees(t *testing.T) {
	setWatchBacklogLimits(t, 4, 4, 4)
	fs := newBasicFilesystem(testDirAbs)
	coarseBefore := testutil.ToFloat64(metricWatchBacklogCoarsened.WithLabelValues(fs.URI()))

	backendChan := make(chan notify.EventInfo, backendBuffer)
	outChan := runWatchLoop(t, fs, backendChan)

	var names []string
	for i := 0; i < 50; i++ {
		names = append(names, filepath.Join("a", "x", fmt.Sprintf("f%02d", i)), filepath.Join("b", fmt.Sprintf("f%02d", i)))
	}
	canary := WatchBarrierCanaryPrefix + "test.tmp"
	names = append(names, canary)
	if !politeBackend(backendChan, names, 3*time.Second) {
		t.Fatal("backend could not deliver: the backend channel was not drained while the consumer was blocked")
	}

	// The first events are queued raw (the queue limit is 4). The rest
	// arrives coarsened to the directories "a" and "b" - possibly in more
	// than one episode, as the loop can drain a set while the backend is
	// still delivering - and the canary comes last.
	var got []string
	for {
		got = append(got, receiveN(t, outChan, 1)...)
		if got[len(got)-1] == canary {
			break
		}
		if len(got) > 40 {
			t.Fatalf("no canary after 40 events: %q", got)
		}
	}
	if !slices.Contains(got, "a") || !slices.Contains(got, "b") {
		t.Fatalf("overflow not coarsened to the directories a and b: %q", got)
	}
	for _, name := range names {
		covered := false
		for _, g := range got {
			if g == name || IsParent(name, g) {
				covered = true
				break
			}
		}
		if !covered {
			t.Fatalf("event %q lost: not covered by %q", name, got)
		}
	}
	for _, name := range got {
		if name == "." {
			t.Fatalf("escalated to the folder root: %q", got)
		}
	}
	if d := testutil.ToFloat64(metricWatchBacklogCoarsened.WithLabelValues(fs.URI())) - coarseBefore; d < 1 {
		t.Errorf("coarsened episodes +%v, want at least 1", d)
	}
}
