// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

func testLine(i int) Line {
	return Line{
		When:    time.Unix(1700000000, 0).Add(time.Duration(i) * time.Second),
		Message: fmt.Sprintf("line %d", i),
		Level:   slog.LevelInfo,
	}
}

func TestLogRingDrainFromEmpty(t *testing.T) {
	t.Parallel()
	r := newLogRing(10)
	lines, next, dropped := r.drainFrom(1)
	if len(lines) != 0 || next != 1 || dropped != 0 {
		t.Errorf("empty ring: lines=%d next=%d dropped=%d", len(lines), next, dropped)
	}
}

func TestLogRingDrainOrder(t *testing.T) {
	t.Parallel()
	r := newLogRing(10)
	for i := range 7 {
		r.append(testLine(i))
	}
	lines, next, dropped := r.drainFrom(1)
	if dropped != 0 || next != 8 {
		t.Fatalf("next=%d dropped=%d", next, dropped)
	}
	for i, l := range lines {
		if l.Message != fmt.Sprintf("line %d", i) {
			t.Errorf("position %d: %q", i, l.Message)
		}
	}
	// Draining again yields nothing new.
	lines, next2, _ := r.drainFrom(next)
	if len(lines) != 0 || next2 != 8 {
		t.Errorf("second drain: lines=%d next=%d", len(lines), next2)
	}
}

func TestLogRingWrapReportsDropped(t *testing.T) {
	t.Parallel()
	r := newLogRing(5)
	for i := range 12 {
		r.append(testLine(i))
	}
	lines, next, dropped := r.drainFrom(1)
	// Ring holds the last 5 (seq 8..12); 7 were overwritten.
	if dropped != 7 {
		t.Errorf("dropped=%d, want 7", dropped)
	}
	if len(lines) != 5 || next != 13 {
		t.Errorf("lines=%d next=%d", len(lines), next)
	}
	if lines[0].Message != "line 7" {
		t.Errorf("oldest retained = %q, want %q", lines[0].Message, "line 7")
	}
}

// blockedWriter blocks all writes until released.
type blockedWriter struct {
	gate    chan struct{}
	release sync.Once
	buf     bytes.Buffer
	mut     sync.Mutex
}

func newBlockedWriter() *blockedWriter { return &blockedWriter{gate: make(chan struct{})} }

func (w *blockedWriter) Write(p []byte) (int, error) {
	<-w.gate
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.buf.Write(p)
}

func (w *blockedWriter) unblock() { w.release.Do(func() { close(w.gate) }) }

func (w *blockedWriter) String() string {
	w.mut.Lock()
	defer w.mut.Unlock()
	return w.buf.String()
}

func TestLogDrainerNeverBlocksProducers(t *testing.T) {
	t.Parallel()
	ring := newLogRing(8)
	w := newBlockedWriter()
	d := startLogDrainer(ring, w, func() LineFormat { return DefaultLineFormat })
	defer d.stop()

	// The drainer is parked inside the blocked writer. Appending far more
	// lines than the ring holds must complete promptly regardless.
	done := make(chan struct{})
	go func() {
		for i := range 100 {
			ring.append(testLine(i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("producers blocked behind the log writer")
	}

	w.unblock()
	d.flush(5 * time.Second)

	out := w.String()
	if !strings.Contains(out, "log lines dropped") {
		t.Error("expected a gap marker after the ring wrapped past the drainer")
	}
	if !strings.Contains(out, "line 99") {
		t.Error("expected the newest line to be written after unblock")
	}
}

func TestLogDrainerWritesAllInOrder(t *testing.T) {
	t.Parallel()
	ring := newLogRing(1024)
	var buf bytes.Buffer
	var mut sync.Mutex
	w := writerFunc(func(p []byte) (int, error) {
		mut.Lock()
		defer mut.Unlock()
		return buf.Write(p)
	})
	d := startLogDrainer(ring, w, func() LineFormat { return DefaultLineFormat })
	defer d.stop()

	const n = 500
	for i := range n {
		ring.append(testLine(i))
	}
	d.flush(5 * time.Second)

	mut.Lock()
	out := buf.String()
	mut.Unlock()
	prev := -1
	for i := range n {
		idx := strings.Index(out, fmt.Sprintf("line %d\n", i))
		if idx < 0 {
			t.Fatalf("line %d missing from output", i)
		}
		if idx < prev {
			t.Fatalf("line %d out of order", i)
		}
		prev = idx
	}
	if strings.Contains(out, "dropped") {
		t.Error("no drops expected with a fast writer and a large ring")
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestLogRingWriteDump(t *testing.T) {
	t.Parallel()
	r := newLogRing(4)
	for i := range 6 {
		r.append(testLine(i))
	}
	var buf bytes.Buffer
	r.WriteDump(&buf, DefaultLineFormat)
	out := buf.String()
	// Ring holds lines 2..5; 2 lines lost to wrap.
	if !strings.Contains(out, "2 log lines dropped") {
		t.Error("dump should lead with a gap marker for the wrapped lines")
	}
	if strings.Contains(out, "line 0\n") || strings.Contains(out, "line 1\n") {
		t.Error("dump must not contain overwritten lines")
	}
	if !strings.Contains(out, "line 5") {
		t.Error("dump missing newest line")
	}
}

func TestLogDrainerFlushTimeout(t *testing.T) {
	t.Parallel()
	ring := newLogRing(8)
	w := newBlockedWriter()
	d := startLogDrainer(ring, w, func() LineFormat { return DefaultLineFormat })

	ring.append(testLine(0))
	// The drainer is stuck in the writer; flush must time out, not hang.
	start := time.Now()
	d.flush(100 * time.Millisecond)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("flush took %v with a blocked writer", elapsed)
	}
	w.unblock()
	d.stop()
}

var _ io.Writer = (*blockedWriter)(nil)
var _ io.Writer = (writerFunc)(nil)
