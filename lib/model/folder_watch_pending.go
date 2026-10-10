// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/scanner"
	"github.com/syncthing/syncthing/lib/watchaggregator"
)

var (
	metricFolderWatchBatchesIntegrated = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "syncthing",
		Subsystem: "model",
		Name:      "folder_watch_batches_integrated_total",
		Help:      "Total number of watch batches dequeued during a scan and covered by its remaining walk, per folder ID",
	}, []string{"folder"})
	metricFolderWatchBatchesDeferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "syncthing",
		Subsystem: "model",
		Name:      "folder_watch_batches_deferred_total",
		Help:      "Total number of watch batches deferred to the pending set instead of being scanned nested inside a running scan, per folder ID",
	}, []string{"folder"})
	metricFolderWatchPendingScans = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "syncthing",
		Subsystem: "model",
		Name:      "folder_watch_pending_scans_total",
		Help:      "Total number of scans of the merged pending watch set, per folder ID",
	}, []string{"folder"})
)

// Watch batches dequeued between the chunks of a running scan are never
// scanned nested inside it. A nested scan ran its own walk and deletion
// sweep inside the outer scan's chunk budget, and it serviced the watch
// channel in turn only if the depth guard let it: measured 2026-11-24 on a
// 3.16M-file home tree, a full-folder batch nested inside a subtree scan
// held the folder in "scanning" for 2 h 38 min, and a 41-path batch nested
// inside another subtree scan took 13 min. Instead a dequeued batch is
// either
//
//   - integrated: the running scan's walk has not reached its paths yet,
//     so the walk (and the deletion sweep after it) covers them, or
//   - deferred: merged into the pending set, which the Serve loop scans as
//     one follow-up scan once the running scan returns - after any pull or
//     other work that is ready at the same time, so the folder alternates
//     between scans and everything else rather than nesting.
//
// The pending set is bounded (fs.BoundedPathSet: past its limit it widens
// paths to their ancestors, never drops them). Only the Serve goroutine
// touches it.

// watchPendingLimit bounds the distinct paths in the pending set; eight
// aggregator windows (512 paths each).
const watchPendingLimit = 8 * 512

type watchPendingSet struct {
	set     *fs.BoundedPathSet
	maxID   uint64
	batches int
}

func newWatchPendingSet() *watchPendingSet {
	return &watchPendingSet{set: fs.NewBoundedPathSet(watchPendingLimit)}
}

func (p *watchPendingSet) add(paths []string, maxID uint64) {
	for _, path := range paths {
		p.set.Add(path)
	}
	if maxID > p.maxID {
		p.maxID = maxID
	}
	p.batches++
}

// scanCoverage describes what the running scan's walk will still visit.
type scanCoverage struct {
	// full: the scan covers the whole folder (subs nil).
	full bool
	// sub: the single subtree the scan covers, when not full.
	sub string
	// walking is true while the walk phase runs; once the deletion sweep
	// started, changed and new files are no longer picked up.
	walking bool
	// frontier is the walk's resume marker: the walk still visits every
	// path that does not precede it in walk order. Empty = nothing walked.
	frontier string
	// integrated collects batches the walk covers; their watermark is
	// persisted (or handed to the pending set) when the scan returns.
	integrated *watchPendingSet
}

// covers reports whether the remaining walk (plus the sweep after it)
// covers path.
func (c *scanCoverage) covers(path string) bool {
	if c == nil || !c.walking {
		return false
	}
	if !c.full && path != c.sub && !fs.IsParent(path, c.sub) {
		return false
	}
	return c.frontier == "" || !scanner.WalkOrderLess(path, c.frontier)
}

// beginScanCoverage installs the coverage record of a scan of subDirs (as
// unified by scanSubdirs) and returns the previous one for restoring.
// Multi-subtree scans are never integrated into: their subtrees are walked
// in string order while the marker speaks walk order, so "ahead of the
// frontier" is not well defined across them.
func (f *folder) beginScanCoverage(subDirs []string) (cur, prev *scanCoverage) {
	prev = f.scanCover
	switch {
	case len(subDirs) == 0:
		cur = &scanCoverage{full: true}
	case len(subDirs) == 1:
		cur = &scanCoverage{sub: subDirs[0]}
	}
	f.scanCover = cur
	return cur, prev
}

// endScanCoverage restores the previous coverage record and settles the
// batches integrated into the finished scan: on success their watermark is
// persisted - unless deferred batches are still pending, whose events may
// precede it, in which case it rides on the pending scan. On failure the
// integrated paths were not necessarily walked and are deferred instead.
func (f *folder) endScanCoverage(cur, prev *scanCoverage, err error) {
	f.scanCover = prev
	if cur == nil || cur.integrated == nil {
		return
	}
	in := cur.integrated
	if err == nil {
		// The walk covered the integrated paths; only their watermark may
		// have to wait for older deferred batches.
		f.persistWatchReplayWatermarkAfterPending(in.maxID)
		return
	}
	f.deferWatchPaths(in.set.Paths(), in.maxID)
}

// persistWatchReplayWatermarkAfterPending persists id, or, while deferred
// watch batches are pending (their events may precede id), hands it to the
// pending scan to persist once that scan succeeds.
func (f *folder) persistWatchReplayWatermarkAfterPending(id uint64) {
	if f.watchPending != nil {
		if id > f.watchPending.maxID {
			f.watchPending.maxID = id
		}
		return
	}
	f.persistWatchReplayWatermark(id)
}

// deferWatchPaths merges paths into the pending set and signals the Serve
// loop.
func (f *folder) deferWatchPaths(paths []string, maxID uint64) {
	if f.watchPending == nil {
		f.watchPending = newWatchPendingSet()
	}
	f.watchPending.add(paths, maxID)
	metricFolderWatchBatchesDeferred.WithLabelValues(f.folderID).Inc()
	select {
	case f.watchPendingReady <- struct{}{}:
	default:
	}
}

// handleWatchBatchMidScan integrates or defers a batch dequeued between the
// chunks of a running scan.
func (f *folder) handleWatchBatchMidScan(ctx context.Context, b watchaggregator.Batch) {
	cover := f.scanCover
	all := cover != nil
	for _, p := range b.Paths {
		if !cover.covers(p) {
			all = false
			break
		}
	}
	if all {
		if cover.integrated == nil {
			cover.integrated = newWatchPendingSet()
		}
		cover.integrated.add(b.Paths, b.MaxEventID)
		metricFolderWatchBatchesIntegrated.WithLabelValues(f.folderID).Inc()
		f.sl.DebugContext(ctx, "Watch batch integrated into the running scan", slog.Int("paths", len(b.Paths)), slog.String("frontier", cover.frontier))
		return
	}
	f.sl.DebugContext(ctx, "Watch batch deferred until the running scan returns", slog.Int("paths", len(b.Paths)))
	f.deferWatchPaths(b.Paths, b.MaxEventID)
}

// servicePendingWatch scans the pending set as one scan. Batches dequeued
// during it are integrated or deferred in turn. On failure the set is put
// back without re-signalling (a persistent folder error must not spin the
// Serve loop); the next deferral, the next watch batch or the shutdown
// drain picks it up.
func (f *folder) servicePendingWatch(ctx context.Context) error {
	p := f.watchPending
	if p == nil {
		return nil
	}
	f.watchPending = nil
	paths := p.set.Paths()
	f.sl.DebugContext(ctx, "Scanning deferred watch batches", slog.Int("batches", p.batches), slog.Int("paths", len(paths)))
	metricFolderWatchPendingScans.WithLabelValues(f.folderID).Inc()
	f.scanningPending = true
	err := f.scanSubdirs(ctx, paths)
	f.scanningPending = false
	if err != nil {
		if f.watchPending == nil {
			f.watchPending = p
		} else {
			f.watchPending.add(paths, p.maxID)
		}
		return err
	}
	f.persistWatchReplayWatermarkAfterPending(p.maxID)
	return nil
}

// drainWatchPending scans the pending set until it stays empty, for the
// btrfs pairing barrier: pairing asserts that every event observed before
// the barrier is in the index, and deferred batches are observed but not
// yet scanned.
func (f *folder) drainWatchPending(ctx context.Context) bool {
	for f.watchPending != nil {
		if ctx.Err() != nil {
			return false
		}
		if err := f.servicePendingWatch(ctx); err != nil {
			return false
		}
	}
	return true
}

// takeWatchPending removes and returns the pending set's paths and
// watermark (nil, 0 when empty).
func (f *folder) takeWatchPending() ([]string, uint64) {
	p := f.watchPending
	if p == nil {
		return nil, 0
	}
	f.watchPending = nil
	return p.set.Paths(), p.maxID
}
