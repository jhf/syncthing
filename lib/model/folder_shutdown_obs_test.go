// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// blockedShutdownStep stands in for the call that ignored the shutdown
// budget on yarlung 2026-11-25 18:39 (still unidentified): it blocks
// without looking at any context.
func blockedShutdownStep(release chan struct{}) { <-release }

// blockedServeWork stands in for the work Serve was inside on yarlung
// 2026-11-26 22:00:41: it does not return on cancellation, so shutdownWork
// never starts.
func blockedServeWork(release chan struct{}) { <-release }

// Work Serve is in the middle of when it is cancelled shares the shutdown
// budget: if it outlives it, the stacks are logged although shutdownWork
// never ran.
//
// Teeth: with the watchdog armed inside shutdownWork (dev.191) no stacks
// line appears, as on yarlung 2026-11-26 22:00:41.
func TestServeCancelOverrunLogsStacks(t *testing.T) {
	var buf lockedBuffer
	oldGrace, oldBudget := shutdownOverrunGrace, folderShutdownBudget
	t.Cleanup(func() { shutdownOverrunGrace, folderShutdownBudget = oldGrace, oldBudget })
	shutdownOverrunGrace = 50 * time.Millisecond
	folderShutdownBudget = 100 * time.Millisecond

	_, f := setupSendReceiveFolder(t)
	f.sl = slog.New(slog.NewTextHandler(&buf, nil))
	// The setup model already ran and stopped this folder once, closing
	// done (doInSync would return at once without reaching Serve) and
	// draining the version-cleanup timer (Serve drains it again at start).
	f.done = make(chan struct{})
	f.versionCleanupTimer = time.NewTimer(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	var serveErr error
	go func() {
		defer close(served)
		serveErr = f.Serve(ctx)
	}()

	release := make(chan struct{})
	entered := make(chan struct{})
	go func() {
		_ = f.doInSync(func(context.Context) error {
			close(entered)
			blockedServeWork(release)
			return nil
		})
	}()
	select {
	case <-entered:
	case <-served:
		t.Fatalf("Serve returned before running the blocking work: %v", serveErr)
	case <-time.After(10 * time.Second):
		t.Fatalf("Serve never ran the blocking work:\n%.6000s", folderGoroutineStacks())
	}
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), "overran") {
		time.Sleep(20 * time.Millisecond)
	}
	close(release)
	<-served
	out := buf.String()
	if !strings.Contains(out, "Folder shutdown work overran its budget") {
		t.Fatalf("no overrun line for work Serve was inside at cancellation:\n%.2000s", out)
	}
	if !strings.Contains(out, "blockedServeWork") {
		t.Fatalf("stacks do not name the blocked call:\n%.2000s", out)
	}
}

// Shutdown work that outlives its budget logs the folder goroutines'
// stacks, naming the blocked call; work that returns in time logs nothing.
//
// Teeth: without the watchdog no stacks line appears, so the next overrun
// would again show only a 53 s silence.
func TestShutdownOverrunLogsStacks(t *testing.T) {
	var buf lockedBuffer
	f := &folder{sl: slog.New(slog.NewTextHandler(&buf, nil))}
	oldGrace := shutdownOverrunGrace
	t.Cleanup(func() { shutdownOverrunGrace = oldGrace })
	shutdownOverrunGrace = 50 * time.Millisecond

	// In time: no dump.
	stop := f.watchShutdownOverrun(time.Now(), 100*time.Millisecond)
	stop()
	time.Sleep(300 * time.Millisecond)
	if strings.Contains(buf.String(), "overran") {
		t.Fatalf("watchdog fired for work that returned in time:\n%s", buf.String())
	}

	// Overrun: the dump names the blocked function.
	release := make(chan struct{})
	defer close(release)
	go blockedShutdownStep(release)
	stop = f.watchShutdownOverrun(time.Now(), 100*time.Millisecond)
	defer stop()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(buf.String(), "overran") {
		time.Sleep(20 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "Folder shutdown work overran its budget") {
		t.Fatalf("no overrun line:\n%s", out)
	}
	if !strings.Contains(out, "blockedShutdownStep") {
		t.Fatalf("stacks do not name the blocked call:\n%.2000s", out)
	}
}

// Each pairing step logs its duration and the context state, so a step
// that overruns the budget is named even without a stack dump.
func TestPairingStepLogsDuration(t *testing.T) {
	var buf lockedBuffer
	f := &folder{sl: slog.New(slog.NewTextHandler(&buf, nil))}
	f.pairingStep(t.Context(), "shutdown", "barrier", func() bool { time.Sleep(20 * time.Millisecond); return false })
	out := buf.String()
	for _, want := range []string{"pairing step done", "step=barrier", "trigger=shutdown", "ok=false", "took="} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in %q", want, out)
		}
	}
}
