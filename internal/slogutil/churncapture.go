// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package slogutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	churnCaptureCooldown  = 10 * time.Minute
	churnCaptureWindow    = 120 * time.Second
	churnCaptureSlots     = 4
	churnCaptureFileBytes = 64 << 20
)

// Capture diagnostics are process counters, never folder/path metric labels.
// No logging is performed by request, so capture cannot recursively trigger it.
type ChurnCaptureCounters struct {
	Accepted, RejectedBusy, RejectedCooldown, Unavailable uint64
	Completed, Failed, Cancelled                          uint64
}
type captureCounters struct {
	accepted, busy, cooldown, unavailable atomic.Uint64
	completed, failed, cancelled          atomic.Uint64
}

var globalCaptureCounters captureCounters
var globalChurnCapture atomic.Pointer[churnCapture]

func ChurnCaptureStats() ChurnCaptureCounters {
	c := &globalCaptureCounters
	return ChurnCaptureCounters{c.accepted.Load(), c.busy.Load(), c.cooldown.Load(), c.unavailable.Load(), c.completed.Load(), c.failed.Load(), c.cancelled.Load()}
}

type churnCapture struct {
	ctx      context.Context
	cancel   context.CancelFunc
	dir      string
	jobs     chan time.Time // One pending-or-active job, enforced by busy.
	done     chan struct{}
	mu       sync.Mutex // TryLock only on producers, never held during worker IO.
	last     time.Time
	busy     atomic.Bool
	now      func() time.Time
	after    func(time.Duration) <-chan time.Time
	dump     func(context.Context, string, time.Time, string) error
	counters *captureCounters
}

func newChurnCapture(ctx context.Context, dir string, counters *captureCounters) *churnCapture {
	ctx, cancel := context.WithCancel(ctx)
	c := &churnCapture{ctx: ctx, cancel: cancel, dir: dir, jobs: make(chan time.Time, 1), done: make(chan struct{}), now: time.Now, after: time.After, counters: counters}
	c.dump = writeChurnCapture
	return c
}

// StartChurnCapture starts the daemon-owned, process-wide capture worker. The
// returned stop cancels timers and cooperatively cancels writes, with a hard
// caller wait limit even when a filesystem syscall cannot be interrupted.
// A timed-out worker continues to own the singleton until it has stopped.
func StartChurnCapture(ctx context.Context, dir string) func(time.Duration) bool {
	c := newChurnCapture(ctx, dir, &globalCaptureCounters)
	if !globalChurnCapture.CompareAndSwap(nil, c) {
		c.cancel()
		return func(time.Duration) bool { return true }
	}
	go c.serve()
	return c.stop
}

// RequestChurnCapture only enqueues a token. It never snapshots a ring, changes
// levels, registers traps, touches disk, logs, or waits for a worker/lock.
func RequestChurnCapture() string {
	c := globalChurnCapture.Load()
	if c == nil {
		globalCaptureCounters.unavailable.Add(1)
		return "unavailable"
	}
	return c.request()
}
func (c *churnCapture) request() string {
	if c.ctx.Err() != nil {
		c.counters.unavailable.Add(1)
		return "unavailable"
	}
	if !c.mu.TryLock() {
		c.counters.busy.Add(1)
		return "rejected_busy"
	}
	defer c.mu.Unlock()
	if c.busy.Load() {
		c.counters.busy.Add(1)
		return "rejected_busy"
	}
	now := c.now()
	if !c.last.IsZero() && now.Sub(c.last) < churnCaptureCooldown {
		c.counters.cooldown.Add(1)
		return "rejected_cooldown"
	}
	c.busy.Store(true)
	select {
	case c.jobs <- now:
		c.last = now
		c.counters.accepted.Add(1)
		return "accepted"
	default:
		c.busy.Store(false)
		c.counters.busy.Add(1)
		return "rejected_busy"
	}
}
func (c *churnCapture) stop(timeout time.Duration) bool {
	c.cancel()
	timer := time.NewTimer(max(0, timeout))
	defer timer.Stop()
	select {
	case <-c.done:
		return true
	case <-timer.C:
		return false
	}
}
func (c *churnCapture) serve() {
	defer func() {
		if c.busy.Swap(false) {
			c.counters.cancelled.Add(1)
		}
		globalChurnCapture.CompareAndSwap(c, nil)
		close(c.done)
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case at := <-c.jobs:
			err := c.capture(at)
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				c.counters.cancelled.Add(1)
			case err != nil:
				c.counters.failed.Add(1)
			default:
				c.counters.completed.Add(1)
			}
			c.busy.Store(false)
		}
	}
}
func capturePath(dir string, slot int, phase string) string {
	return filepath.Join(dir, fmt.Sprintf("capture-%d-%s.log", slot, phase))
}

func (c *churnCapture) capture(at time.Time) error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(c.dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("capture directory is not a real directory")
	}
	if err := os.Chmod(c.dir, 0o700); err != nil {
		return err
	}
	// Only these fixed managed names are touched. Unknown files are never pruned.
	slot := 0
	var oldest time.Time
	for i := range churnCaptureSlots {
		var newest time.Time
		for _, phase := range []string{"pre", "post"} {
			p := capturePath(c.dir, i, phase)
			if err := os.Remove(p + ".tmp"); err != nil && !os.IsNotExist(err) {
				return err
			}
			st, err := os.Lstat(p)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if err == nil && st.ModTime().After(newest) {
				newest = st.ModTime()
			}
		}
		if i == 0 || newest.Before(oldest) {
			slot, oldest = i, newest
		}
	}
	for _, phase := range []string{"pre", "post"} {
		if err := os.Remove(capturePath(c.dir, slot, phase)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if err := c.dump(c.ctx, capturePath(c.dir, slot, "pre"), at, "pre"); err != nil {
		return err
	}
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-c.after(churnCaptureWindow):
	}
	return c.dump(c.ctx, capturePath(c.dir, slot, "post"), at, "post")
}

type captureWriter struct {
	ctx       context.Context
	out       io.Writer
	remaining int64
}

func (w *captureWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.remaining {
		return 0, errors.New("capture file byte limit")
	}
	n, err := w.out.Write(p)
	w.remaining -= int64(n)
	return n, err
}

// Unlike the generic trap/panic dump, this private worker writer propagates
// write errors, checks cancellation between lines and atomically publishes only
// closed, complete files. Existing synchronous dump/trap behavior is untouched.
func writeChurnCapture(ctx context.Context, path string, at time.Time, phase string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tmp := path + ".tmp"
	fd, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = fd.Close(); _ = os.Remove(tmp) }()
	w := &captureWriter{ctx: ctx, out: fd, remaining: churnCaptureFileBytes}
	if _, err = fmt.Fprintf(w, "=== churn capture schema=1 id=%s phase=%s snapshot=%s ===\n", at.UTC().Format(time.RFC3339Nano), phase, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	for _, section := range []struct {
		name string
		ring *logRing
	}{{"overview", globalOverview}, {"detail", globalRing}} {
		lines, _, dropped := section.ring.drainFrom(1)
		if _, err = fmt.Fprintf(w, "=== %s ring: lines=%d dropped=%d ===\n", section.name, len(lines), dropped); err != nil {
			return err
		}
		for _, line := range lines {
			if int64(len(line.Message)) > w.remaining {
				return errors.New("capture file byte limit")
			}
			if _, err = line.WriteTo(w, DefaultLineFormat); err != nil {
				return err
			}
		}
	}
	if _, err = fmt.Fprintln(w, "=== capture.complete ==="); err != nil {
		return err
	}
	if err = fd.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
