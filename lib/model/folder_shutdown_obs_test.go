// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
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
