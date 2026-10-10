// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var metricFolderPullRescanNames = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "syncthing",
	Subsystem: "model",
	Name:      "folder_pull_rescan_names_total",
	Help:      "Total number of names the puller asked to rescan that were rescanned, per folder ID",
}, []string{"folder"})

// The puller asks for a rescan whenever disk and index disagree about an
// item it is about to change (errModified): a file gone from disk that the
// index still lists, a file edited since it was indexed. The pull backs off
// and retries once the rescan has recorded the local state.
//
// Upstream ran that rescan through Scan, which waits for the initial scan to
// finish. With pulls running between the chunks of a scan, that wait is a
// livelock for the whole of a long initial scan: measured on yarlung
// 2026-11-25 18:47 to 11-26 16:30, a 21h43m initial scan during which 1,823
// pull passes failed the same deletes again, 6,471,843 "Failed to delete
// file" lines over 1,988 files. The rescans could not run until the scan
// ended, and the deletes went through the moment it did.
//
// So the requested names are queued here and rescanned at the next chance:
// the next chunk boundary of a running scan, or the Serve loop when no scan
// runs. Folding them into the running walk would not do: a deletion is only
// recorded by the sweep after the walk, hours away in a long scan. A
// boundary rescans at most pullRescanBatch names, so its cost stays bounded
// by the work it does; the rest wait for the next boundary.

// pullRescanBatch bounds the names one boundary rescans.
const pullRescanBatch = 4096

// queuePullRescan records names the puller wants rescanned and signals the
// Serve loop. Safe from any goroutine.
func (f *folder) queuePullRescan(names []string) {
	if len(names) == 0 {
		return
	}
	f.pullRescanMut.Lock()
	if f.pullRescanNames == nil {
		f.pullRescanNames = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		f.pullRescanNames[name] = struct{}{}
	}
	f.pullRescanMut.Unlock()
	f.signalPullRescan()
}

func (f *folder) signalPullRescan() {
	select {
	case f.pullRescanReady <- struct{}{}:
	default:
	}
}

// takePullRescan removes and returns up to limit queued names, sorted, and
// whether any remain.
func (f *folder) takePullRescan(limit int) ([]string, bool) {
	f.pullRescanMut.Lock()
	defer f.pullRescanMut.Unlock()
	names := make([]string, 0, min(limit, len(f.pullRescanNames)))
	for name := range f.pullRescanNames {
		if len(names) == limit {
			break
		}
		names = append(names, name)
	}
	for _, name := range names {
		delete(f.pullRescanNames, name)
	}
	slices.Sort(names)
	return names, len(f.pullRescanNames) > 0
}

// requeuePullRescan puts names back without signalling: a scan that failed
// on a persistent folder error must not spin the Serve loop. The next
// request or the next boundary picks them up.
func (f *folder) requeuePullRescan(names []string) {
	f.pullRescanMut.Lock()
	defer f.pullRescanMut.Unlock()
	if f.pullRescanNames == nil {
		f.pullRescanNames = make(map[string]struct{}, len(names))
	}
	for _, name := range names {
		f.pullRescanNames[name] = struct{}{}
	}
}

// servicePullRescan rescans up to pullRescanBatch queued names. It runs in
// the Serve goroutine, from the Serve loop or from a chunk boundary of a
// running scan; the scan it runs never services the queue again
// (pullRescanActive), so the nesting is one level deep. The bool reports
// whether a rescan ran.
func (f *folder) servicePullRescan(ctx context.Context) (bool, error) {
	if f.pullRescanActive {
		return false, nil
	}
	names, more := f.takePullRescan(pullRescanBatch)
	if more {
		f.signalPullRescan()
	}
	if len(names) == 0 {
		return false, nil
	}
	// A request for the folder root is a full scan, which must not run
	// nested inside another scan: hand it to the scan timer.
	if slices.ContainsFunc(names, isRootPath) {
		names = slices.DeleteFunc(names, isRootPath)
		f.ScheduleScan()
		if len(names) == 0 {
			return false, nil
		}
	}
	// Watch batches stay queued during this scan: one integrated into it
	// could persist its watermark while a batch integrated into the running
	// outer scan is still unwalked. The outer scan's next boundary takes them.
	prevHold := f.watchHold
	f.pullRescanActive, f.watchHold = true, true
	defer func() { f.pullRescanActive, f.watchHold = false, prevHold }()
	f.sl.DebugContext(ctx, "Rescanning names the puller found changed on disk", slog.Int("names", len(names)), slog.Bool("more", more))
	if err := f.scanSubdirs(ctx, names); err != nil {
		if ctx.Err() == nil {
			f.requeuePullRescan(names)
		}
		return true, err
	}
	metricFolderPullRescanNames.WithLabelValues(f.folderID).Add(float64(len(names)))
	return true, nil
}

// pullRescanCollector returns the channel the puller reports names to
// rescan on, and a finish function that closes it and returns once the
// names are queued. Finishing before the pull returns means the scan chunk
// boundary right after the pull already sees them.
func (f *folder) pullRescanCollector() (chan<- string, func()) {
	ch := make(chan string)
	done := make(chan struct{})
	go func() {
		defer close(done)
		seen := make(map[string]struct{})
		names := make([]string, 0)
		for name := range ch {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
		f.queuePullRescan(names)
	}()
	return ch, func() {
		close(ch)
		<-done
	}
}

func isRootPath(p string) bool {
	return p == "" || p == "." || p == "/"
}
