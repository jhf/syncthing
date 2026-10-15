// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package fs

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/syncthing/notify"
)

// captureWatchEscalationLog replaces the escalation log sink for the test
// and returns the captured lines.
func captureWatchEscalationLog(t *testing.T, interval time.Duration) func() []string {
	t.Helper()
	var mut sync.Mutex
	var lines []string
	oldLog, oldInterval := watchEscalationLog, watchEscalationLogInterval
	watchEscalationLog = func(args ...any) {
		mut.Lock()
		lines = append(lines, fmt.Sprint(args...))
		mut.Unlock()
	}
	watchEscalationLogInterval = interval
	t.Cleanup(func() {
		watchEscalationLog, watchEscalationLogInterval = oldLog, oldInterval
	})
	return func() []string {
		mut.Lock()
		defer mut.Unlock()
		return append([]string(nil), lines...)
	}
}

// TestNoteWatchRootEscalationCountsAndRateLimits pins the accounting: every
// escalation is counted, the log line is rate-limited per root and reason,
// and the next logged line reports how many were suppressed in between.
func TestNoteWatchRootEscalationCountsAndRateLimits(t *testing.T) {
	lines := captureWatchEscalationLog(t, time.Hour)
	root := t.Name()

	for range 3 {
		NoteWatchRootEscalation(root, WatchEscalationBackendDropped)
	}
	NoteWatchRootEscalation(root, WatchEscalationBufferOverflow)

	if got := testutil.ToFloat64(metricWatchRootEscalations.WithLabelValues(root, WatchEscalationBackendDropped)); got != 3 {
		t.Errorf("backend_dropped count = %v, want 3", got)
	}
	if got := testutil.ToFloat64(metricWatchRootEscalations.WithLabelValues(root, WatchEscalationBufferOverflow)); got != 1 {
		t.Errorf("buffer_overflow count = %v, want 1", got)
	}
	if got := len(lines()); got != 2 {
		t.Fatalf("logged %d lines, want 2 (one per reason within the interval): %q", got, lines())
	}

	// After the interval the next line carries the suppressed count.
	watchEscalationLogInterval = 0
	NoteWatchRootEscalation(root, WatchEscalationBackendDropped)
	got := lines()
	if len(got) != 3 {
		t.Fatalf("logged %d lines, want 3: %q", len(got), got)
	}
	if want := fmt.Sprint("root", root, "reason", WatchEscalationBackendDropped, "suppressedSinceLast", 2); got[2] != want {
		t.Errorf("line = %q, want %q", got[2], want)
	}
}

// TestWatchOverflowCountsEscalation pins that a backend buffer overflow -
// a lossy escalation to a full-folder scan - is counted and logged instead
// of only reaching a debug line. The buffer is filled before the loop
// starts, so the overflow is deterministic.
func TestWatchOverflowCountsEscalation(t *testing.T) {
	lines := captureWatchEscalationLog(t, 0)
	fs := newBasicFilesystem(testDirAbs)
	before := testutil.ToFloat64(metricWatchRootEscalations.WithLabelValues(fs.URI(), WatchEscalationBufferOverflow))

	backendChan := make(chan notify.EventInfo, backendBuffer)
	for i := 0; i < backendBuffer; i++ {
		backendChan <- fakeEventInfo(filepath.Join(testDirAbs, strconv.Itoa(i)))
	}
	outChan := make(chan Event)
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		fs.watchLoop(ctx, ".", []string{testDirAbs}, backendChan, outChan, errChan, fakeMatcher{})
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	select {
	case ev := <-outChan:
		if ev.Name != "." {
			t.Fatalf("first event after overflow = %q, want the root", ev.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the overflow root event")
	}

	after := testutil.ToFloat64(metricWatchRootEscalations.WithLabelValues(fs.URI(), WatchEscalationBufferOverflow))
	if after-before != 1 {
		t.Errorf("buffer_overflow escalations %v -> %v, want +1", before, after)
	}
	if len(lines()) != 1 {
		t.Errorf("logged %q, want one escalation line", lines())
	}
}
