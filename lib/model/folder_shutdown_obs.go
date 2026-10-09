// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package model

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Observability for the folder's shutdown work (shutdownWork): the btrfs
// pairing on yarlung 2026-11-25 18:39:43 logged its snapshot, then nothing
// until the supervisors abandoned the folder 53 s later, past the 50 s
// budget. Neither "watch barrier not reached" nor the deferred rmsnap
// appeared, so some step blocked without honouring the budget. Each step
// now logs its duration, and a watchdog logs the goroutine stacks of the
// shutdown work once the budget has passed while it is still running: the
// next occurrence names the blocking call.

// shutdownOverrunGrace is how long past the budget the watchdog waits
// before dumping stacks: a step that honours ctx returns well inside it.
var shutdownOverrunGrace = 2 * time.Second

// shutdownStackDumpMax caps the logged stack text.
const shutdownStackDumpMax = 64 << 10

// pairingStep times one step of the baseline pairing at INFO. Steps are
// few (one pairing per 30 min, a handful of steps), so this is not noise.
func (f *folder) pairingStep(ctx context.Context, trigger, step string, fn func() bool) bool {
	t0 := time.Now()
	ok := fn()
	f.sl.InfoContext(ctx, "Btrfs baseline: pairing step done", "trigger", trigger, "step", step, "ok", ok, "took", time.Since(t0).Round(time.Millisecond), "ctxErr", ctxErrString(ctx))
	return ok
}

func ctxErrString(ctx context.Context) string {
	if err := ctx.Err(); err != nil {
		return err.Error()
	}
	return ""
}

// pairingScanSlow is the duration from which a scan inside a pairing step
// is logged individually.
var pairingScanSlow = time.Second

// pairingScan scans one watch batch inside a pairing step and logs it when
// it is slow: a barrier made of many scans otherwise shows only its total.
func (f *folder) pairingScan(ctx context.Context, step string, paths []string) error {
	t0 := time.Now()
	err := f.scanSubdirs(ctx, paths)
	if d := time.Since(t0); d >= pairingScanSlow {
		first := ""
		if len(paths) > 0 {
			first = paths[0]
		}
		f.sl.InfoContext(ctx, "Btrfs baseline: slow scan inside pairing", "step", step, "paths", len(paths), "first", first, "took", d.Round(time.Millisecond), "error", err, "ctxErr", ctxErrString(ctx))
	}
	return err
}

// watchShutdownOverrun starts the watchdog for shutdown work that started
// at start under a budget. The returned stop must be called when the work
// returns.
func (f *folder) watchShutdownOverrun(start time.Time, budget time.Duration) (stop func()) {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTimer(budget + shutdownOverrunGrace)
		defer t.Stop()
		select {
		case <-done:
			return
		case <-t.C:
		}
		f.sl.Info("Folder shutdown work overran its budget; goroutine stacks follow",
			"budget", budget, "elapsed", time.Since(start).Round(time.Millisecond),
			"stacks", folderGoroutineStacks())
	}()
	return func() { once.Do(func() { close(done) }) }
}

// folderGoroutineStacks returns the stacks of goroutines in lib/model and
// below (the folder's Serve goroutine and anything it waits on), capped at
// shutdownStackDumpMax. Unrelated goroutines (connections, GUI) are
// dropped to keep the line readable.
func folderGoroutineStacks() string {
	buf := make([]byte, 4<<20)
	n := runtime.Stack(buf, true)
	var out strings.Builder
	for _, g := range bytes.Split(buf[:n], []byte("\n\n")) {
		if !bytes.Contains(g, []byte("syncthing/lib/model.")) {
			continue
		}
		if out.Len()+len(g)+2 > shutdownStackDumpMax {
			out.WriteString("\n[truncated]")
			break
		}
		out.Write(g)
		out.WriteString("\n\n")
	}
	return out.String()
}
