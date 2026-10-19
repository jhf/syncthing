// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
)

// The log ring is the primary sink for all log lines (the logcat model):
// producers append to the in-memory ring in O(1) under a mutex and never
// touch the output file. A single drainer goroutine follows the ring with a
// cursor and performs all file I/O; if the ring wraps past the cursor the
// drainer emits a gap marker with the number of lost lines. On panic the
// ring can be dumped to a separate file, preserving the last N lines even
// when the main log stream is lost or interleaved with the crash.
//
// Producers never block on the drainer, the disk, or the log consumer.

const defaultLogRingLines = 1 << 15 // 32768 lines, a few MB of RAM

type logRing struct {
	mut   sync.Mutex
	lines []Line
	seq   uint64        // total lines ever appended; slot for seq s is (s-1)%cap
	poke  chan struct{} // cap 1 wakeup for the drainer
}

func newLogRing(capacity int) *logRing {
	if capacity <= 0 {
		capacity = defaultLogRingLines
	}
	return &logRing{
		lines: make([]Line, capacity),
		poke:  make(chan struct{}, 1),
	}
}

func logRingCapacityFromEnv() int {
	if s := os.Getenv("STLOGRINGLINES"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return defaultLogRingLines
}

// append adds a line to the ring and wakes the drainer. Never blocks on I/O.
func (r *logRing) append(l Line) {
	r.mut.Lock()
	r.seq++
	r.lines[(r.seq-1)%uint64(len(r.lines))] = l
	r.mut.Unlock()
	select {
	case r.poke <- struct{}{}:
	default:
	}
}

// drainFrom returns the lines with sequence numbers >= next, the following
// sequence number to continue from, and how many lines were lost because
// the ring wrapped past next before they were drained.
func (r *logRing) drainFrom(next uint64) (lines []Line, newNext uint64, dropped uint64) {
	r.mut.Lock()
	defer r.mut.Unlock()
	total := r.seq
	if total == 0 || next > total {
		return nil, next, 0
	}
	oldest := uint64(1)
	if total > uint64(len(r.lines)) {
		oldest = total - uint64(len(r.lines)) + 1
	}
	if next < oldest {
		dropped = oldest - next
		next = oldest
	}
	lines = make([]Line, 0, total-next+1)
	for s := next; s <= total; s++ {
		lines = append(lines, r.lines[(s-1)%uint64(len(r.lines))])
	}
	return lines, total + 1, dropped
}

// WriteDump writes the entire current ring contents to w, oldest first.
func (r *logRing) WriteDump(w io.Writer, f LineFormat) {
	lines, _, dropped := r.drainFrom(1)
	if dropped > 0 {
		writeGapMarker(w, f, dropped)
	}
	for i := range lines {
		_, _ = lines[i].WriteTo(w, f)
	}
}

func writeGapMarker(w io.Writer, f LineFormat, dropped uint64) {
	marker := Line{
		When:    time.Now(),
		Level:   slog.LevelWarn,
		Message: fmt.Sprintf("... %d log lines dropped (log writer behind, ring wrapped) ...", dropped),
	}
	_, _ = marker.WriteTo(w, f)
}

// logDrainer follows the ring and performs all output I/O.
type logDrainer struct {
	ring   *logRing
	out    io.Writer
	format func() LineFormat
	next   uint64

	flushReq chan chan struct{} // request: drain fully, then close the given channel
	quit     chan struct{}
	done     chan struct{}
}

func startLogDrainer(ring *logRing, out io.Writer, format func() LineFormat) *logDrainer {
	d := &logDrainer{
		ring:     ring,
		out:      out,
		format:   format,
		next:     1,
		flushReq: make(chan chan struct{}),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go d.serve()
	return d
}

func (d *logDrainer) drain() {
	lines, next, dropped := d.ring.drainFrom(d.next)
	d.next = next
	f := d.format()
	if dropped > 0 {
		writeGapMarker(d.out, f, dropped)
	}
	for i := range lines {
		_, _ = lines[i].WriteTo(d.out, f)
	}
}

func (d *logDrainer) serve() {
	defer close(d.done)
	for {
		select {
		case ack := <-d.flushReq:
			d.drain()
			close(ack)
		case <-d.quit:
			d.drain()
			return
		case <-d.ring.poke:
			d.drain()
		}
	}
}

// flush waits until everything currently in the ring has been written, or
// the timeout expires.
func (d *logDrainer) flush(timeout time.Duration) {
	ack := make(chan struct{})
	select {
	case d.flushReq <- ack:
	case <-d.done:
		return
	case <-time.After(timeout):
		return
	}
	select {
	case <-ack:
	case <-d.done:
	case <-time.After(timeout):
	}
}

// stop drains one final time and stops the drainer.
func (d *logDrainer) stop() {
	close(d.quit)
	<-d.done
}
