// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The consumer side of watch root escalations (lib/fs/watch_escalation.go
// is the producer side): where a full-folder watch batch was dequeued.
// 2026-11-24 on the Mac a root batch serviced between the chunks of a
// subtree scan ran a full walk plus a full deletion sweep nested inside it
// and the folder stayed in "scanning" for 2 h 38 min without a single line
// in the log saying so. Batches are no longer scanned nested (see
// folder_watch_pending.go); the counter shows where root batches go.

// slowScanLogThreshold is the duration from which a finished scan logs one
// INFO line. Ordinary watch-triggered subtree scans finish in well under a
// second; a minute-class scan is the event worth an evidence trail.
const slowScanLogThreshold = time.Minute

var metricFolderRootWatchBatches = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "syncthing",
	Subsystem: "model",
	Name:      "folder_root_watch_batches_total",
	Help:      "Total number of full-folder watch batches dequeued, per folder ID and where they were dequeued (serve, between-chunks-skipped, between-chunks-deferred, pending)",
}, []string{"folder", "where"})

// isRootWatchBatch reports whether a watch batch asks for a full-folder
// scan, in either of its spellings.
func isRootWatchBatch(paths []string) bool {
	for _, p := range paths {
		if p == "." || p == "" {
			return true
		}
	}
	return false
}

// noteRootWatchBatch counts and logs a dequeued full-folder watch batch.
// where is serve (scanned by the Serve loop), between-chunks-skipped
// (dropped because a full-tree scan is already running),
// between-chunks-deferred (merged into the pending set) or pending (the
// pending set collapsed to the root and is being scanned).
func (f *folder) noteRootWatchBatch(ctx context.Context, where string) {
	metricFolderRootWatchBatches.WithLabelValues(f.folderID, where).Inc()
	f.sl.InfoContext(ctx, "Full-folder watch batch dequeued", slog.String("where", where))
}

func firstOrEmpty(s []string) string {
	if len(s) == 0 {
		return ""
	}
	return s[0]
}
