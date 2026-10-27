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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func captureReady(t *testing.T, ready func() bool) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for !ready() {
		select {
		case <-timeout:
			t.Fatal("capture barrier timed out")
		default:
			runtime.Gosched()
		}
	}
}
func immediateCaptureTimer(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func TestChurnCaptureProducerBounds(t *testing.T) {
	c := newChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"), new(captureCounters))
	defer c.cancel()
	var wg sync.WaitGroup
	for range 1000 {
		wg.Go(func() { c.request() })
	}
	wg.Wait()
	if len(c.jobs) != 1 || c.counters.accepted.Load() != 1 || c.counters.busy.Load() != 999 {
		t.Fatal("queue or concurrent acceptance exceeded one pending/active job")
	}
	c.mu.Lock()
	result := make(chan string, 1)
	go func() { result <- c.request() }()
	select {
	case got := <-result:
		if got != "rejected_busy" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("producer waited for capture lock")
	}
	c.mu.Unlock()
	if _, err := os.Stat(c.dir); !os.IsNotExist(err) {
		t.Fatal("enqueue touched disk")
	}
}

func TestChurnCaptureActiveCooldownAndCancellation(t *testing.T) {
	c := newChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"), new(captureCounters))
	now := time.Now()
	c.now = func() time.Time { return now }
	entered, release := make(chan struct{}), make(chan struct{})
	c.dump = func(ctx context.Context, _ string, _ time.Time, phase string) error {
		if phase == "pre" {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	c.after = immediateCaptureTimer
	go c.serve()
	t.Cleanup(func() { c.stop(time.Second) })
	if c.request() != "accepted" {
		t.Fatal("initial request rejected")
	}
	<-entered
	for range 1000 {
		if c.request() != "rejected_busy" {
			t.Fatal("active capture accepted another request")
		}
	}
	close(release)
	captureReady(t, func() bool { return !c.busy.Load() })
	if c.request() != "rejected_cooldown" || c.counters.completed.Load() != 1 {
		t.Fatal("cooldown or completion accounting wrong")
	}
	// The minimum cooldown boundary is inclusive, and a failed capture still
	// spends its token rather than permitting a storage-error retry storm.
	now = now.Add(churnCaptureCooldown)
	c.dump = func(context.Context, string, time.Time, string) error { return errors.New("storage failure") }
	if c.request() != "accepted" {
		t.Fatal("cooldown boundary did not allow new capture")
	}
	captureReady(t, func() bool { return !c.busy.Load() })
	if c.counters.failed.Load() != 1 || c.request() != "rejected_cooldown" {
		t.Fatal("failed capture bypassed cooldown")
	}
	if !c.stop(time.Second) || c.request() != "unavailable" {
		t.Fatal("worker did not shut down")
	}
}

func TestChurnCaptureStopBoundAndSingleton(t *testing.T) {
	stop := StartChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"))
	c := globalChurnCapture.Load()
	if c == nil {
		t.Fatal("worker missing")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.dump = func(context.Context, string, time.Time, string) error {
		close(entered)
		<-release
		return context.Canceled
	}
	if RequestChurnCapture() != "accepted" {
		t.Fatal("request rejected")
	}
	<-entered
	if stop(10 * time.Millisecond) {
		t.Fatal("blocked filesystem simulation reported stopped")
	}
	if globalChurnCapture.Load() != c {
		t.Fatal("timed-out worker relinquished singleton")
	}
	another := StartChurnCapture(t.Context(), filepath.Join(t.TempDir(), "unused"))
	if globalChurnCapture.Load() != c {
		t.Fatal("second active worker started")
	}
	another(time.Millisecond)
	close(release)
	if !stop(time.Second) || globalChurnCapture.Load() != nil {
		t.Fatal("worker failed to release singleton after real stop")
	}
}

func TestChurnCaptureCancelPostWindowKeepsOnlyCompletePre(t *testing.T) {
	c := newChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"), new(captureCounters))
	waiting := make(chan struct{})
	c.after = func(d time.Duration) <-chan time.Time {
		if d != 120*time.Second {
			t.Errorf("post window %s", d)
		}
		close(waiting)
		return make(chan time.Time)
	}
	go c.serve()
	if c.request() != "accepted" {
		t.Fatal("initial request rejected")
	}
	<-waiting
	if !c.stop(time.Second) || c.counters.cancelled.Load() != 1 {
		t.Fatal("post timer not cancelled")
	}
	files, err := os.ReadDir(c.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name() != "capture-0-pre.log" {
		t.Fatalf("partial file published: %v", files)
	}
	data, err := os.ReadFile(filepath.Join(c.dir, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "=== capture.complete ===\n") {
		t.Fatal("pre dump incomplete")
	}
}

func TestChurnCaptureRotationPrivateAndBounded(t *testing.T) {
	c := newChurnCapture(t.Context(), filepath.Join(t.TempDir(), "capture"), new(captureCounters))
	defer c.cancel()
	c.after = immediateCaptureTimer
	for i := range 9 {
		at := time.Unix(1000+int64(i), 0)
		if err := c.capture(at); err != nil {
			t.Fatal(err)
		}
		files, err := os.ReadDir(c.dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != min((i+1)*2, 8) {
			t.Fatalf("retention grew to %d", len(files))
		}
		for _, phase := range []string{"pre", "post"} {
			p := capturePath(c.dir, i%4, phase)
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), "id="+at.UTC().Format(time.RFC3339Nano)) || !strings.HasSuffix(string(data), "=== capture.complete ===\n") {
				t.Fatal("oldest slot not replaced with complete new pair")
			}
			st, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			if st.Mode().Perm() != 0o600 {
				t.Fatalf("file mode %o", st.Mode().Perm())
			}
			if st.Size() > churnCaptureFileBytes {
				t.Fatal("file byte cap exceeded")
			}
			if err := os.Chtimes(p, at, at); err != nil {
				t.Fatal(err)
			}
		}
	}
	st, err := os.Stat(c.dir)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode %o", st.Mode().Perm())
	}
}

type cancellingCaptureContext struct {
	context.Context
	calls atomic.Int64
}

func (c *cancellingCaptureContext) Err() error {
	if c.calls.Add(1) >= 3 {
		return context.Canceled
	}
	return nil
}
func TestChurnCaptureAtomicCancellationAndWriterLimits(t *testing.T) {
	p := filepath.Join(t.TempDir(), "capture.log")
	ctx := &cancellingCaptureContext{Context: context.Background()}
	if err := writeChurnCapture(ctx, p, time.Now(), "pre"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, path := range []string{p, p + ".tmp"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("cancelled partial file remained")
		}
	}
	var buf strings.Builder
	w := &captureWriter{ctx: context.Background(), out: &buf, remaining: 3}
	if _, err := w.Write([]byte("four")); err == nil || buf.Len() != 0 {
		t.Fatal("capture byte budget failed")
	}
	if _, err := w.Write([]byte("yes")); err != nil {
		t.Fatal(err)
	}
}

func TestChurnCaptureRejectSymlinkAndPreserveUnknownFiles(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	c := newChurnCapture(t.Context(), link, new(captureCounters))
	defer c.cancel()
	c.after = immediateCaptureTimer
	if err := c.capture(time.Now()); err == nil {
		t.Fatal("symlink capture directory followed")
	}
	c.dir = target
	unknown := filepath.Join(target, "keep.txt")
	if err := os.WriteFile(unknown, []byte("user data"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if err := os.WriteFile(capturePath(target, i, "pre")+".tmp", []byte("crash residue"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.capture(time.Now()); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatal(fmt.Sprintf("unexpected managed retention: %v", files))
	}
	if data, err := os.ReadFile(unknown); err != nil || string(data) != "user data" {
		t.Fatal("unrelated file changed")
	}
}
